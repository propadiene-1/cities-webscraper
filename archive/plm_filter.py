"""
plm_filter.py
=============
Filters election + demographics data for Paris, Lyon, Marseille.

These cities vote by arrondissement, so we match both the city-level
code and all arrondissement codes.

Update INPUT_FILES at the top, then run.
"""

import pandas as pd
from pathlib import Path

BASE_DIR = Path("/Users/propadiene/cloned-repos/cities-webscraper")
OUT_DIR  = BASE_DIR / "plm_outputs"

# ── Files to filter ───────────────────────────────────────────────────────────
INPUT_FILES = [
    BASE_DIR / "new_france_joined_outputs/france_joined_2020/joined_plus_1000_tour1_2020.json",
    BASE_DIR / "new_france_joined_outputs/france_joined_2020/joined_plus_1000_tour2_2020.json",
    BASE_DIR / "new_france_joined_outputs/france_joined_2014/joined_plus_1000_tour1_2014.json",
    BASE_DIR / "new_france_joined_outputs/france_joined_2014/joined_plus_1000_tour2_2014.json",
]

# ── Which commune codes belong to each city ───────────────────────────────────
CITY_CODES = {
    "Paris":     [f"75056SR{str(i).zfill(2)}" for i in range(1, 21)],
    "Lyon":      [f"69123SR{str(i).zfill(2)}" for i in range(1, 10)],
    "Marseille": [f"13055SR{str(i).zfill(2)}" for i in range(1, 9)],  # SR01–SR08
}

# Flat lookup: commune_code → city name
CODE_TO_CITY = {code: city for city, codes in CITY_CODES.items() for code in codes}


if __name__ == "__main__":
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    all_rows = []

    for path in INPUT_FILES:
        if not path.exists():
            print(f"Skipping (not found): {path.name}")
            continue

        df = pd.read_json(path)
        df["commune_code"] = df["commune_code"].astype(str)
        plm = df[df["commune_code"].isin(CODE_TO_CITY)].copy()
        plm["city"] = plm["commune_code"].map(CODE_TO_CITY)

        # Extract year and tour from filename e.g. "joined_plus_1000_tour1_2020"
        parts = path.stem.split("_")
        plm["year"] = int(parts[-1])
        plm["tour"] = int(parts[-2].replace("tour", ""))

        print(f"{path.name}: {len(plm):,} rows — {plm['city'].value_counts().to_dict()}")
        all_rows.append(plm)

    if not all_rows:
        print("No data found — check INPUT_FILES paths.")
    else:
        combined = pd.concat(all_rows, ignore_index=True)

        # One combined file (all cities, all years, all tours)
        combined.to_csv(OUT_DIR / "plm_all.csv", index=False, encoding="utf-8-sig")
        combined.to_json(OUT_DIR / "plm_all.json", orient="records", force_ascii=False, indent=2)

        # One file per year/tour (all cities)
        for year in sorted(combined["year"].unique()):
            for tour in sorted(combined["tour"].unique()):
                subset = combined[(combined["year"] == year) & (combined["tour"] == tour)]
                if subset.empty:
                    continue
                filename = f"plm_{year}_tour{tour}"
                subset.to_csv(OUT_DIR / f"{filename}.csv", index=False, encoding="utf-8-sig")
                subset.to_json(OUT_DIR / f"{filename}.json", orient="records", force_ascii=False, indent=2)
        
        print(f"\nDONE: {len(combined):,} total rows saved to {OUT_DIR}")
        print(combined.groupby(["city", "year", "tour"]).size().to_string())