"""Convert raw marathon results into a time-ordered event stream.

Reads a Boston Marathon results CSV in its original wide format (one row per
runner with one column per checkpoint split) and writes a long-format CSV
containing one row per runner-checkpoint, sorted by elapsed time. The output
is consumed by the producer, which replays it into Kafka.

Input:  data/results.csv  (Kaggle dataset rojour/boston-results)
Output: data/events.csv
"""
import pandas as pd

from config import CHECKPOINTS, RACE_ID

INPUT_PATH = "data/results.csv"
OUTPUT_PATH = "data/events.csv"


def parse_split(value) -> int | None:
    """Convert a split string in 'H:MM:SS' format to a number of seconds.

    Returns None for missing, blank, or malformed values.
    """
    if not isinstance(value, str) or value.strip() in ("", "-"):
        return None
    try:
        hours, minutes, seconds = value.split(":")
        return int(hours) * 3600 + int(minutes) * 60 + int(seconds)
    except ValueError:
        return None


def find_column(df: pd.DataFrame, candidates: list[str]) -> str:
    """Return the first column name from candidates that exists in df.

    Raises KeyError if none of the candidates are present.
    """
    for name in candidates:
        if name in df.columns:
            return name
    raise KeyError(
        f"None of the expected columns {candidates} were found in the input file."
    )


def main():
    """Read the input CSV, melt it into long format, and write the output CSV."""
    df = pd.read_csv(INPUT_PATH)
    df.columns = [c.strip() for c in df.columns]

    # Column names vary slightly between years of the Boston dataset
    bib_col = find_column(df, ["Bib", "bib"])
    name_col = find_column(df, ["Name", "name"])
    age_col = find_column(df, ["Age", "age"])
    gender_col = find_column(df, ["M/F", "gender"])

    rows = []
    for _, runner in df.iterrows():
        for checkpoint_label, checkpoint_km in CHECKPOINTS.items():
            if checkpoint_label not in df.columns:
                continue
            elapsed = parse_split(runner[checkpoint_label])
            if elapsed is None or elapsed <= 0:
                continue
            rows.append({
                "race_id": RACE_ID,
                "runner_id": str(runner[bib_col]),
                "runner_name": str(runner[name_col]).strip(),
                "age": int(runner[age_col]) if pd.notna(runner[age_col]) else None,
                "gender": str(runner[gender_col]),
                "checkpoint_label": checkpoint_label,
                "checkpoint_km": checkpoint_km,
                "elapsed_seconds": elapsed,
            })

    events = (
        pd.DataFrame(rows)
        .sort_values("elapsed_seconds")
        .reset_index(drop=True)
    )
    events.to_csv(OUTPUT_PATH, index=False)

    print(f"Wrote {len(events):,} events to {OUTPUT_PATH}")
    print(f"Unique runners: {events['runner_id'].nunique():,}")
    print(
        f"Elapsed time range: "
        f"{events['elapsed_seconds'].min()}s to {events['elapsed_seconds'].max()}s"
    )


if __name__ == "__main__":
    main()