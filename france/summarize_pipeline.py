"""
summarize_pipeline.py
=====================
Post-pipeline diagnostics. Five reports under summary_stats/:

  pipeline_drops.csv  — one row per (year, file):
      source_rows        rows entering process_*.py
      dropped            rows process_*.py wrote to dropped_outputs/
      tour1_carryover    subset of `dropped` that are tour-1 losers
                         (tour-2 files only; 0 for tour-1 files)
      unexplained_drops  dropped - tour1_carryover (true no-match drops)
      pct_unexplained    unexplained_drops / source_rows
      real_communes      distinct real-INSEE communes in the joined output
      pct_non_commune    Z-prefix codes (Interior Ministry election IDs
                         that don't map to any INSEE commune) / real_communes
      pct_no_age_gender  real communes missing BTT census / real_communes
      pct_no_income      real communes missing FILOSOFI income / real_communes

  pipeline_missingness.csv — one row per (year, file, column): NaN count + pct.

  unmatched_communes.csv — one row per (year, file, stage, commune_code).
    Stage is "census", "income", or "non_commune_code".

  unmatched_real_communes.csv — same as above but with non_commune_code
    rows removed (real INSEE communes that failed to match census/income).

  commune_coverage.csv — wide grid: one row per commune_code, one column
    per (year, file), with `years_present` summary.
"""

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parent
OUT_DIR  = ROOT / "summary_stats"
JOINED   = ROOT / "newest_france_joined_outputs"

# (year, label, candidate_outputs path, joined output path, pop_col, drop_reason)
#
# Short drop_reason values (one per file). Three distinct categories:
#   "no_match"             — a registered/candidat row had no matching result
#   "commune_not_in_result" — whole commune missing from results (2014 plus t2)
#   "tour1_loser_or_no_match" — tour-2 only; mostly tour-1 candidates who
#                               didn't advance, mixed with genuine no-match
TARGETS = [
    ("2008", "plus_1000_tour1",
        ROOT / "france_2008/candidate_outputs/plus_1000_tour1_2008.csv",
        JOINED / "france_joined_2008/joined_plus_1000_tour1_2008.csv",
        "P08_POP", "no_match"),
    ("2008", "plus_1000_tour2",
        ROOT / "france_2008/candidate_outputs/plus_1000_tour2_2008.csv",
        JOINED / "france_joined_2008/joined_plus_1000_tour2_2008.csv",
        "P08_POP", "tour1_loser_or_no_match"),
    ("2014", "plus_1000_tour1",
        ROOT / "france_2014/candidate_outputs/plus_1000_tour1_2014.csv",
        JOINED / "france_joined_2014/joined_plus_1000_tour1_2014.csv",
        "P14_POP", "no_match"),
    ("2014", "plus_1000_tour2",
        ROOT / "france_2014/candidate_outputs/plus_1000_tour2_2014.csv",
        JOINED / "france_joined_2014/joined_plus_1000_tour2_2014.csv",
        "P14_POP", "commune_not_in_result"),
    ("2014", "less_1000_tour1",
        ROOT / "france_2014/candidate_outputs/less_1000_tour1_2014.csv",
        JOINED / "france_joined_2014/joined_less_1000_tour1_2014.csv",
        "P14_POP", "no_match"),
    ("2014", "less_1000_tour2",
        ROOT / "france_2014/candidate_outputs/less_1000_tour2_2014.csv",
        JOINED / "france_joined_2014/joined_less_1000_tour2_2014.csv",
        "P14_POP", "no_match"),
    ("2020", "plus_1000_tour1",
        ROOT / "france_2020/candidate_outputs/plus_1000_tour1_2020.csv",
        JOINED / "france_joined_2020/joined_plus_1000_tour1_2020.csv",
        "P19_POP", "no_match"),
    ("2020", "plus_1000_tour2",
        ROOT / "france_2020/candidate_outputs/plus_1000_tour2_2020.csv",
        JOINED / "france_joined_2020/joined_plus_1000_tour2_2020.csv",
        "P19_POP", "tour1_loser_or_no_match"),
    ("2020", "less_1000_tour1",
        ROOT / "france_2020/candidate_outputs/less_1000_tour1_2020.csv",
        JOINED / "france_joined_2020/joined_less_1000_tour1_2020.csv",
        "P19_POP", "no_match"),
    ("2020", "less_1000_tour2",
        ROOT / "france_2020/candidate_outputs/less_1000_tour2_2020.csv",
        JOINED / "france_joined_2020/joined_less_1000_tour2_2020.csv",
        "P19_POP", "tour1_loser_or_no_match"),
    ("2026", "tour1",
        ROOT / "france_2026/candidate_outputs/tour1_2026.csv",
        JOINED / "france_joined_2026/joined_tour1_2026.csv",
        "P22_POP", "no_match"),
    ("2026", "tour2",
        ROOT / "france_2026/candidate_outputs/tour2_2026.csv",
        JOINED / "france_joined_2026/joined_tour2_2026.csv",
        "P22_POP", "tour1_loser_or_no_match"),
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


def is_non_commune_code(code: str) -> bool:
    """True for codes that don't correspond to any real INSEE commune.

    Verified against BTT 2008/2014/2019/2022:
      Z* prefix:  0 / 314 distinct codes appear as CODGEO -> 100% not commune.
                  These are Interior Ministry election IDs for overseas
                  territories (e.g. ZA101 = Les Abymes); the underlying
                  commune exists under a different INSEE code (97101) that
                  the election data simply doesn't carry.

    97x/98x are NOT in this filter — they ARE valid INSEE commune codes
    (overseas departments and collectivities). BTT happens to publish 97x
    but not 98x; either way they're real communes, so unmatched 97x/98x
    rows are legitimate drops and stay in the main count.
    """
    return isinstance(code, str) and code.startswith("Z")


def dropped_path(year: str, label: str) -> Path:
    if year == "2026":
        # process_2026.py writes dropped_outputs/dropped_{label}_2026.csv
        return ROOT / f"france_{year}/dropped_outputs/dropped_{label}_2026.csv"
    return ROOT / f"france_{year}/dropped_outputs/dropped_{label}_{year}.csv"


def count_rows(path: Path) -> int:
    """Row count via pandas so multiline-quoted CSV fields aren't miscounted."""
    if not path.exists():
        return 0
    return len(pd.read_csv(path, dtype=str, low_memory=False, usecols=[0]))


def summarize() -> None:
    drops_rows, miss_rows, unmatched_rows, coverage_rows = [], [], [], []

    for year, label, cand_path, joined_path, pop_col, drop_reason in TARGETS:
        print(f"  scanning {year} {label}")
        df = pd.read_csv(joined_path, dtype={"commune_code": str}, low_memory=False)

        n_final   = len(df)
        n_dropped = count_rows(dropped_path(year, label))
        # source_rows is the raw row count entering process_*.py: rows that
        # survived (== rows in joined output, since merge is a left join) plus
        # rows the process script wrote to dropped_outputs/.
        n_source  = n_final + n_dropped

        # Split rows by whether the commune_code is a real commune code at all.
        # Z* codes are Interior Ministry election IDs that don't correspond to
        # any INSEE commune; they can never match census/income and are not
        # counted as "drops" in the main columns.
        non_commune_mask  = df["commune_code"].apply(is_non_commune_code)
        non_commune_rows  = int(non_commune_mask.sum())
        non_commune_codes = sorted(df.loc[non_commune_mask, "commune_code"].dropna().unique())

        real_df = df[~non_commune_mask]
        real_commune_count = real_df["commune_code"].nunique()
        census_mask = real_df[pop_col].isna()
        income_mask = real_df["median_income"].isna()
        census_communes = sorted(real_df.loc[census_mask, "commune_code"].dropna().unique())
        income_communes = sorted(real_df.loc[income_mask, "commune_code"].dropna().unique())

        def pct(num: int, den: int) -> float:
            return round(num / den * 100, 2) if den else 0.0

        # Tour-2 attrition split: how many dropped rows were just tour-1 losers?
        # For tour-1 files this concept doesn't apply, so carryover stays 0 and
        # all drops are "unexplained" (i.e. genuine no-match registrations).
        tour1_carryover   = 0
        unexplained_drops = n_dropped
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
                tour1_carryover   = hit
                unexplained_drops = n_dropped - hit

        drops_rows.append({
            "year": year,
            "file": label,
            "drop_reason":          drop_reason,
            # process_*.py stage
            "source_rows":          n_source,
            "dropped":              n_dropped,
            "tour1_carryover":      tour1_carryover,
            "unexplained_drops":    unexplained_drops,
            "pct_unexplained":      pct(unexplained_drops, n_source),
            # commune-level gaps (denominator = distinct real communes in this file)
            "real_communes":        real_commune_count,
            "pct_non_commune":      pct(len(non_commune_codes), real_commune_count),
            "pct_no_age_gender":    pct(len(census_communes),  real_commune_count),
            "pct_no_income":        pct(len(income_communes),  real_commune_count),
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
            })
        for code in income_communes:
            unmatched_rows.append({
                "year": year, "file": label, "stage": "income",
                "commune_code": code, "commune_name": name_lookup.get(code, ""),
            })
        for code in non_commune_codes:
            unmatched_rows.append({
                "year": year, "file": label, "stage": "non_commune_code",
                "commune_code": code, "commune_name": name_lookup.get(code, ""),
            })

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(drops_rows).to_csv(
        OUT_DIR / "pipeline_drops.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(miss_rows).to_csv(
        OUT_DIR / "pipeline_missingness.csv", index=False, encoding="utf-8-sig")
    unmatched_df = pd.DataFrame(unmatched_rows)
    unmatched_df.to_csv(
        OUT_DIR / "unmatched_communes.csv", index=False, encoding="utf-8-sig")
    # Same data, narrowed to real INSEE communes (drop non_commune_code rows
    # since those are election-system IDs that can't match by design).
    unmatched_df[unmatched_df["stage"] != "non_commune_code"].to_csv(
        OUT_DIR / "unmatched_real_communes.csv",
        index=False, encoding="utf-8-sig")

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

    print(f"\nWrote 5 reports to {OUT_DIR}")
    print(pd.DataFrame(drops_rows).to_string(index=False))


if __name__ == "__main__":
    summarize()
