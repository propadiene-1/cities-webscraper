"""
summarize_pipeline.py
=====================
Post-pipeline diagnostics. Reads each candidate_outputs file and its
corresponding final joined output, and reports:

  summary_stats/pipeline_drops.csv
    One row per (year, file). Source row count, rows dropped in
    process_*.py, rows in the final joined output, and how many rows /
    communes failed to match BTT census or FILOSOFI income.

  summary_stats/pipeline_missingness.csv
    One row per (year, file, column). NaN count and percentage.

  summary_stats/unmatched_communes.csv
    One row per (year, file, stage, commune_code). Stage is "census" or
    "income". Lets you see exactly which communes lost demographics or
    income data and where.

  summary_stats/commune_coverage.csv
    One row per commune_code. Marks for each (year, file) whether it
    appears in the final output at all and whether it has full census /
    income data. Surfaces communes that show up in some elections but
    are absent in others.
"""

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parent
OUT_DIR  = ROOT / "summary_stats"
JOINED   = ROOT / "newest_france_joined_outputs"

# (year, label, candidate_outputs path, joined output path, pop_col, drop_reason)
#
# drop_reason mirrors the print line each process_*.py emits when writing its
# dropped_outputs/ file. "tour1-eliminated-or-no-match" tour-2 files are
# dominated by candidates who didn't advance from tour 1 (not data loss); the
# tour1_attrition_rows column below tries to quantify that split.
TARGETS = [
    ("2008", "plus_1000_tour1",
        ROOT / "france_2008/candidate_outputs/plus_1000_tour1_2008.csv",
        JOINED / "france_joined_2008/joined_plus_1000_tour1_2008.csv",
        "P08_POP",
        "candidat with no result row"),
    ("2008", "plus_1000_tour2",
        ROOT / "france_2008/candidate_outputs/plus_1000_tour2_2008.csv",
        JOINED / "france_joined_2008/joined_plus_1000_tour2_2008.csv",
        "P08_POP",
        "candidat without tour-2 result (eliminated in tour 1 or no match)"),
    ("2014", "plus_1000_tour1",
        ROOT / "france_2014/candidate_outputs/plus_1000_tour1_2014.csv",
        JOINED / "france_joined_2014/joined_plus_1000_tour1_2014.csv",
        "P14_POP",
        "candidat with no result row"),
    ("2014", "plus_1000_tour2",
        ROOT / "france_2014/candidate_outputs/plus_1000_tour2_2014.csv",
        JOINED / "france_joined_2014/joined_plus_1000_tour2_2014.csv",
        "P14_POP",
        "candidature commune absent from results"),
    ("2014", "less_1000_tour1",
        ROOT / "france_2014/candidate_outputs/less_1000_tour1_2014.csv",
        JOINED / "france_joined_2014/joined_less_1000_tour1_2014.csv",
        "P14_POP",
        "registration with no result match"),
    ("2014", "less_1000_tour2",
        ROOT / "france_2014/candidate_outputs/less_1000_tour2_2014.csv",
        JOINED / "france_joined_2014/joined_less_1000_tour2_2014.csv",
        "P14_POP",
        "registration with no tour-2 result match"),
    ("2020", "plus_1000_tour1",
        ROOT / "france_2020/candidate_outputs/plus_1000_tour1_2020.csv",
        JOINED / "france_joined_2020/joined_plus_1000_tour1_2020.csv",
        "P19_POP",
        "registration with no result match"),
    ("2020", "plus_1000_tour2",
        ROOT / "france_2020/candidate_outputs/plus_1000_tour2_2020.csv",
        JOINED / "france_joined_2020/joined_plus_1000_tour2_2020.csv",
        "P19_POP",
        "registration without tour-2 result (eliminated in tour 1 or no match)"),
    ("2020", "less_1000_tour1",
        ROOT / "france_2020/candidate_outputs/less_1000_tour1_2020.csv",
        JOINED / "france_joined_2020/joined_less_1000_tour1_2020.csv",
        "P19_POP",
        "registration with no result match"),
    ("2020", "less_1000_tour2",
        ROOT / "france_2020/candidate_outputs/less_1000_tour2_2020.csv",
        JOINED / "france_joined_2020/joined_less_1000_tour2_2020.csv",
        "P19_POP",
        "registration without tour-2 result (eliminated in tour 1 or no match)"),
    ("2026", "tour1",
        ROOT / "france_2026/candidate_outputs/tour1_2026.csv",
        JOINED / "france_joined_2026/joined_tour1_2026.csv",
        "P22_POP",
        "candidature with no result row"),
    ("2026", "tour2",
        ROOT / "france_2026/candidate_outputs/tour2_2026.csv",
        JOINED / "france_joined_2026/joined_tour2_2026.csv",
        "P22_POP",
        "candidature without tour-2 result (eliminated in tour 1 or no match)"),
]

# Mapping: tour2 file → its tour1 final-output path. Used to split tour2
# drops into "tour1 eliminations" vs genuinely unmatched.
TOUR2_TO_TOUR1 = {
    "2008 plus_1000_tour2": JOINED / "france_joined_2008/joined_plus_1000_tour1_2008.csv",
    "2014 plus_1000_tour2": JOINED / "france_joined_2014/joined_plus_1000_tour1_2014.csv",
    "2014 less_1000_tour2": JOINED / "france_joined_2014/joined_less_1000_tour1_2014.csv",
    "2020 plus_1000_tour2": JOINED / "france_joined_2020/joined_plus_1000_tour1_2020.csv",
    "2020 less_1000_tour2": JOINED / "france_joined_2020/joined_less_1000_tour1_2020.csv",
    "2026 tour2":           JOINED / "france_joined_2026/joined_tour1_2026.csv",
}


def is_dom_tom(code: str) -> bool:
    """True if a commune code is in an INSEE overseas territory.
    INSEE uses '97x'/'98x'; the Interior Ministry uses 'ZA/ZB/ZC/ZD/ZS/ZM/ZN/ZW/ZX'.
    """
    if not isinstance(code, str) or not code:
        return False
    return code.startswith(("97", "98")) or code[0] == "Z"


def dropped_path(year: str, label: str) -> Path:
    if year == "2026":
        # process_2026.py writes dropped_outputs/dropped_{label}_2026.csv
        return ROOT / f"france_{year}/dropped_outputs/dropped_{label}_2026.csv"
    return ROOT / f"france_{year}/dropped_outputs/dropped_{label}_{year}.csv"


def count_rows(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for _ in open(path, encoding="utf-8-sig")) - 1  # minus header


def summarize() -> None:
    drops_rows, miss_rows, unmatched_rows, coverage_rows = [], [], [], []

    for year, label, cand_path, joined_path, pop_col, drop_reason in TARGETS:
        print(f"  scanning {year} {label}")
        df = pd.read_csv(joined_path, dtype={"commune_code": str}, low_memory=False)

        n_final     = len(df)
        n_source    = count_rows(cand_path)
        n_dropped   = count_rows(dropped_path(year, label))

        census_mask = df[pop_col].isna()
        income_mask = df["median_income"].isna()
        census_communes = sorted(df.loc[census_mask, "commune_code"].dropna().unique())
        income_communes = sorted(df.loc[income_mask, "commune_code"].dropna().unique())

        # Tour-2 attrition split: how many dropped rows were just tour-1 losers?
        tour1_attrition = ""
        genuine_drops   = ""
        key = f"{year} {label}"
        if key in TOUR2_TO_TOUR1 and n_dropped > 0:
            dpath = dropped_path(year, label)
            if dpath.exists():
                dropped = pd.read_csv(dpath, dtype={"commune_code": str},
                                      low_memory=False, usecols=lambda c: c in
                                      ("commune_code", "last_name", "first_name"))
                tour1 = pd.read_csv(TOUR2_TO_TOUR1[key], dtype={"commune_code": str},
                                    low_memory=False,
                                    usecols=["commune_code", "last_name", "first_name"])
                tour1_keys = set(zip(tour1["commune_code"], tour1["last_name"],
                                     tour1["first_name"]))
                drop_keys  = list(zip(dropped["commune_code"], dropped["last_name"],
                                      dropped["first_name"]))
                hit = sum(1 for k in drop_keys if k in tour1_keys)
                tour1_attrition = hit
                genuine_drops   = n_dropped - hit

        # DOM-TOM (overseas) split of income misses
        dom_tom_communes  = [c for c in income_communes if is_dom_tom(c)]
        metro_communes    = [c for c in income_communes if not is_dom_tom(c)]
        dom_tom_mask      = income_mask & df["commune_code"].apply(is_dom_tom)
        dom_tom_rows      = int(dom_tom_mask.sum())

        drops_rows.append({
            "year": year,
            "file": label,
            "drop_reason":                  drop_reason,
            "source_rows":                  n_source,
            "dropped_in_process":           n_dropped,
            "tour1_attrition_rows":         tour1_attrition,
            "genuine_drops":                genuine_drops,
            "final_rows":                   n_final,
            "communes_in_final":            df["commune_code"].nunique(),
            "census_unmatched_rows":        int(census_mask.sum()),
            "census_unmatched_communes":    len(census_communes),
            "income_unmatched_rows":        int(income_mask.sum()),
            "income_unmatched_communes":    len(income_communes),
            "income_unmatched_dom_tom_rows":      dom_tom_rows,
            "income_unmatched_dom_tom_communes":  len(dom_tom_communes),
            "income_unmatched_metro_communes":    len(metro_communes),
        })

        for col in df.columns:
            n_miss = int(df[col].isna().sum())
            miss_rows.append({
                "year": year,
                "file": label,
                "column": col,
                "total_rows":   n_final,
                "missing_rows": n_miss,
                "pct_missing":  round(n_miss / n_final * 100, 2) if n_final else 0.0,
            })

        # One row per commune in this file, computed via groupby (cheap).
        per_commune = df.groupby("commune_code").agg(
            commune_name=("commune_name", "first"),
            has_census=(pop_col, lambda s: s.notna().any()),
            has_income=("median_income", lambda s: s.notna().any()),
        ).reset_index()

        for row in per_commune.itertuples(index=False):
            coverage_rows.append({
                "commune_code": row.commune_code,
                "commune_name": row.commune_name,
                "year": year,
                "file": label,
                "has_census": bool(row.has_census),
                "has_income": bool(row.has_income),
            })

        name_lookup = dict(zip(per_commune["commune_code"], per_commune["commune_name"]))
        for code in census_communes:
            unmatched_rows.append({
                "year": year, "file": label, "stage": "census",
                "commune_code": code, "commune_name": name_lookup.get(code, ""),
                "is_dom_tom": is_dom_tom(code),
            })
        for code in income_communes:
            unmatched_rows.append({
                "year": year, "file": label, "stage": "income",
                "commune_code": code, "commune_name": name_lookup.get(code, ""),
                "is_dom_tom": is_dom_tom(code),
            })

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(drops_rows).to_csv(
        OUT_DIR / "pipeline_drops.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(miss_rows).to_csv(
        OUT_DIR / "pipeline_missingness.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(unmatched_rows).to_csv(
        OUT_DIR / "unmatched_communes.csv", index=False, encoding="utf-8-sig")

    cov = pd.DataFrame(coverage_rows)
    pivot = cov.assign(present=1).pivot_table(
        index=["commune_code", "commune_name"],
        columns=["year", "file"],
        values="present",
        fill_value=0,
    )
    pivot.columns = [f"{y}_{f}" for y, f in pivot.columns]
    pivot["years_present"] = pivot.gt(0).sum(axis=1)
    pivot.reset_index().to_csv(
        OUT_DIR / "commune_coverage.csv", index=False, encoding="utf-8-sig")

    print(f"\nWrote 4 reports to {OUT_DIR}")
    print(pd.DataFrame(drops_rows).to_string(index=False))


if __name__ == "__main__":
    summarize()
