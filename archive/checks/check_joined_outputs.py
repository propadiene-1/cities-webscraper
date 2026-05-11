"""
check_joined.py
===============
Cross-check old vs. new joined election + demographics outputs.
"""

import pandas as pd
from pathlib import Path

BASE_DIR = Path("/Users/propadiene/cloned-repos/cities-webscraper")

# adjust files to check here
FILES = [
    (
        BASE_DIR / "archive/france_joined_outputs/france_joined_2020/joined_plus_1000_tour1_2020.csv",
        BASE_DIR / "new_france_joined_outputs/france_joined_2020/joined_plus_1000_tour1_2020.csv",
    ),
    (
        BASE_DIR / "archive/france_joined_outputs/france_joined_2020/joined_plus_1000_tour2_2020.csv",
        BASE_DIR / "new_france_joined_outputs/france_joined_2020/joined_plus_1000_tour2_2020.csv",
    ),
    (
        BASE_DIR / "archive/france_joined_outputs/france_joined_2020/joined_less_1000_tour1_2020.csv",
        BASE_DIR / "new_france_joined_outputs/france_joined_2020/joined_less_1000_tour1_2020.csv",
    ),
    (
        BASE_DIR / "archive/france_joined_outputs/france_joined_2020/joined_less_1000_tour2_2020.csv",
        BASE_DIR / "new_france_joined_outputs/france_joined_2020/joined_less_1000_tour2_2020.csv",
    ),
    (
        BASE_DIR / "archive/france_joined_outputs/france_joined_2014/joined_plus_1000_tour1_2014.csv",
        BASE_DIR / "new_france_joined_outputs/france_joined_2014/joined_plus_1000_tour1_2014.csv",
    ),
    (
        BASE_DIR / "archive/france_joined_outputs/france_joined_2014/joined_plus_1000_tour2_2014.csv",
        BASE_DIR / "new_france_joined_outputs/france_joined_2014/joined_plus_1000_tour2_2014.csv",
    ),
    (
        BASE_DIR / "archive/france_joined_outputs/france_joined_2014/joined_less_1000_tour1_2014.csv",
        BASE_DIR / "new_france_joined_outputs/france_joined_2014/joined_less_1000_tour1_2014.csv",
    ),
    (
        BASE_DIR / "archive/france_joined_outputs/france_joined_2014/joined_less_1000_tour2_2014.csv",
        BASE_DIR / "new_france_joined_outputs/france_joined_2014/joined_less_1000_tour2_2014.csv",
    ),
]

DEMO_COLS = ["pct_female", "pct_unemployed", "pct_edu_higher", "pct_age_0_14"]


for pre_path, post_path in FILES:
    print(f"\n{'='*60}")
    print(f"{post_path.name}")
    print(f"{'='*60}")

    if not pre_path.exists():
        print(f"  ✗ Pre-join file not found: {pre_path.name}")
        continue
    if not post_path.exists():
        print(f"  ✗ Post-join file not found: {post_path.name}")
        continue

    pre  = pd.read_csv(pre_path, dtype={"commune_code": str})
    post = pd.read_csv(post_path, dtype={"commune_code": str})

    # Check row counts
    match = "MATCHED" if len(pre) == len(post) else "X"
    print(f"  {match} Rows - pre: {len(pre):,}  post: {len(post):,}")

    # Check null rates on demographic columns
    available = [c for c in DEMO_COLS if c in post.columns]
    if available:
        null_rates = post[available].isna().mean() * 100
        print(f"  Null rates:")
        for col, rate in null_rates.items():
            flag = "WARNING" if rate > 10 else ""
            print(f"    {flag} {col}: {rate:.1f}%")
    else:
        print("  X No demographic columns found")
        continue

    # Check values are between 0 - 100
    out_of_range = {}
    for col in available:
        bad = post[col].dropna()
        bad = bad[(bad < 0) | (bad > 100)]
        if len(bad) > 0:
            out_of_range[col] = len(bad)
    if out_of_range:
        print(f"  FAILED: Out-of-range values (not 0-100): {out_of_range}")
    else:
        print(f"  SUCCESS: All demographic values in valid range (0-100)")

    # Summary stats
    print(f"  Summary stats:")
    print(post[available].describe().loc[["mean", "min", "max"]].round(1).to_string())