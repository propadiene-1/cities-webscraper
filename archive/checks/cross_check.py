import pandas as pd
from pathlib import Path

BASE_DIR = Path("/Users/propadiene/cloned-repos/cities-webscraper")

files = [
    "plus_1000_tour1_2020.csv",
    "plus_1000_tour2_2020.csv",
    "less_1000_tour1_2020.csv",
    "less_1000_tour2_2020.csv",
    "plus_1000_tour1_2014.csv",
    "plus_1000_tour2_2014.csv",
    "less_1000_tour1_2014.csv",
    "less_1000_tour2_2014.csv",
]

for filename in files:
    year = "2020" if "2020" in filename else "2014"
    old_path = BASE_DIR / f"france_{year}/archived_candidate_outputs/{filename}"
    new_path = BASE_DIR / f"france_{year}/candidate_outputs/{filename}"

    if not old_path.exists() or not new_path.exists():
        print(f"{filename}: missing — old={old_path.exists()} new={new_path.exists()}")
        continue

    old = pd.read_csv(old_path, dtype=str)
    new = pd.read_csv(new_path, dtype=str)

    added   = set(new["commune_code"].unique()) - set(old["commune_code"].unique())
    removed = set(old["commune_code"].unique()) - set(new["commune_code"].unique())

    print(f"\n{filename}")
    print(f"  Rows     — old: {len(old):,}  new: {len(new):,}  diff: {len(new)-len(old):+,}")
    print(f"  Communes — old: {old['commune_code'].nunique():,}  new: {new['commune_code'].nunique():,}")
    if added:
        print(f"  New communes ({len(added)}): {sorted(added)[:10]}")
    if removed:
        print(f"  Removed communes ({len(removed)}): {sorted(removed)[:10]}")