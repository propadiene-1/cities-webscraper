"""
run_pipeline.py
===============
Run the full election pipeline end-to-end.

For each (year, tour, commune_type) variant, this temporarily rewrites the
YEAR / TOUR constants in the relevant process_*.py, runs the script as a
subprocess, then restores the original file at the end (or on any error).

Order:
  1. process_2008.py            TOUR=1, TOUR=2
  2. process_plus_1000.py       (2014,1) (2014,2) (2020,1) (2020,2)
  3. process_less_1000.py       (2014,1) (2014,2) (2020,1) (2020,2)
  4. process_2026.py            TOUR=1, TOUR=2
  5. merge_btt_census.py        (single run — iterates all 12 candidate_outputs,
                                 attaches BTT census + FILOSOFI income)
"""

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent

PROCESS_SCRIPTS = [
    "process_2008.py",
    "process_2026.py",
    "process_plus_1000.py",
    "process_less_1000.py",
]


def set_constants(script: Path, year: int | None = None, tour: int | None = None) -> None:
    text = script.read_text()
    if year is not None:
        text = re.sub(r"^YEAR\s*=.*$", f"YEAR = {year}", text, flags=re.M)
    if tour is not None:
        text = re.sub(r"^TOUR\s*=.*$", f"TOUR = {tour}   # 1 or 2", text, flags=re.M)
    script.write_text(text)


def run(script: Path, **constants) -> None:
    set_constants(script, **constants)
    label = " ".join(f"{k}={v}" for k, v in constants.items())
    banner = f"{script.name}" + (f"  ({label})" if label else "")
    print("\n" + "=" * 70)
    print(f"RUN: {banner}")
    print("=" * 70, flush=True)
    subprocess.run([sys.executable, str(script)], check=True, cwd=ROOT)


def main() -> None:
    backups = {name: (ROOT / name).read_text() for name in PROCESS_SCRIPTS}
    try:
        for tour in (1, 2):
            run(ROOT / "process_2008.py", tour=tour)

        for year in (2014, 2020):
            for tour in (1, 2):
                run(ROOT / "process_plus_1000.py", year=year, tour=tour)

        for year in (2014, 2020):
            for tour in (1, 2):
                run(ROOT / "process_less_1000.py", year=year, tour=tour)

        for tour in (1, 2):
            run(ROOT / "process_2026.py", tour=tour)

        run(ROOT / "merge_btt_census.py")
        run(ROOT / "summarize_pipeline.py")
    finally:
        for name, original in backups.items():
            (ROOT / name).write_text(original)

    print("\nPIPELINE DONE.")


if __name__ == "__main__":
    main()
