"""
preprocess.py
---------------
Read the Boston Marathon CSV (wide format, one row per runner with split columns)
and produce a long-format, time-ordered events CSV that the producer can replay.

Input : data/results.csv  (Kaggle rojour/boston-results, e.g. marathon_results_2015.csv)
Output: data/events.csv   (one row per runner-checkpoint, sorted by elapsed_seconds)
"""
import pandas as pd
from config import CHECKPOINTS, RACE_ID

INPUT = "data/results.csv"
OUTPUT = "data/events.csv"


def parse_split(s: str) -> int | None:
    """'1:23:45' or '0:05:12' -> seconds. Missing/invalid -> None."""
    if not isinstance(s, str) or s.strip() in ("", "-"):
        return None
    try:
        h, m, sec = s.split(":")
        return int(h) * 3600 + int(m) * 60 + int(sec)
    except ValueError:
        return None


def main():
    df = pd.read_csv(INPUT)
    # Kaggle file has either 'Bib' or 'bib' depending on year; normalize.
    df.columns = [c.strip() for c in df.columns]
    bib_col = "Bib" if "Bib" in df.columns else "bib"
    name_col = "Name" if "Name" in df.columns else "name"
    age_col = "Age" if "Age" in df.columns else "age"
    gender_col = "M/F" if "M/F" in df.columns else "gender"

    rows = []
    for _, r in df.iterrows():
        for split_col, km in CHECKPOINTS.items():
            if split_col not in df.columns:
                continue
            secs = parse_split(r[split_col])
            if secs is None or secs <= 0:
                continue
            rows.append({
                "race_id": RACE_ID,
                "runner_id": str(r[bib_col]),
                "runner_name": str(r[name_col]).strip(),
                "age": int(r[age_col]) if pd.notna(r[age_col]) else None,
                "gender": str(r[gender_col]),
                "checkpoint_label": split_col,
                "checkpoint_km": km,
                "elapsed_seconds": secs,
            })

    events = pd.DataFrame(rows).sort_values("elapsed_seconds").reset_index(drop=True)
    events.to_csv(OUTPUT, index=False)
    print(f"Wrote {len(events):,} events to {OUTPUT}")
    print(f"Runners: {events['runner_id'].nunique():,}")
    print(f"Race duration (fastest→slowest): "
          f"{events['elapsed_seconds'].min()}s → {events['elapsed_seconds'].max()}s")


if __name__ == "__main__":
    main()
