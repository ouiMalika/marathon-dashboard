"""Convert raw marathon results into a time-ordered event stream.

Reads a Boston Marathon results CSV in its original wide format (one row per
runner with one column per checkpoint split) and writes a long-format CSV
containing one row per runner-checkpoint, sorted by elapsed time. The output
is consumed by the producer, which replays it into Kafka.

Input:  data/results.csv  (Kaggle dataset rojour/boston-results)
Output: data/events.csv
"""
import pandas as pd

from config import CHECKPOINTS, RACES

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
    """Read each race's CSV, melt it into long format, and write the combined output."""
    all_rows = []

    for race_id, input_path in RACES.items():
        print(f"Reading {race_id} from {input_path}")
        df = pd.read_csv(input_path)
        df.columns = [c.strip() for c in df.columns]

        bib_col = find_column(df, ["Bib", "bib"])
        name_col = find_column(df, ["Name", "name"])
        age_col = find_column(df, ["Age", "age"])
        gender_col = find_column(df, ["M/F", "gender"])

        race_rows = 0
        for _, runner in df.iterrows():
            for checkpoint_label, checkpoint_km in CHECKPOINTS.items():
                if checkpoint_label not in df.columns:
                    continue
                elapsed = parse_split(runner[checkpoint_label])
                if elapsed is None or elapsed <= 0:
                    continue
                all_rows.append({
                    "race_id": race_id,
                    "runner_id": str(runner[bib_col]),
                    "runner_name": str(runner[name_col]).strip(),
                    "age": int(runner[age_col]) if pd.notna(runner[age_col]) else None,
                    "gender": str(runner[gender_col]),
                    "checkpoint_label": checkpoint_label,
                    "checkpoint_km": checkpoint_km,
                    "elapsed_seconds": elapsed,
                })
                race_rows += 1
        print(f"  {race_rows:,} events from {race_id}")

    events = (
        pd.DataFrame(all_rows)
        .sort_values("elapsed_seconds")
        .reset_index(drop=True)
    )
    events.to_csv(OUTPUT_PATH, index=False)

    print(f"\nWrote {len(events):,} total events to {OUTPUT_PATH}")
    print(f"Races: {events['race_id'].nunique()}")
    print(f"Unique runners: {events['runner_id'].nunique():,}")
    print(
        f"Elapsed time range: "
        f"{events['elapsed_seconds'].min()}s to {events['elapsed_seconds'].max()}s"
    )


if __name__ == "__main__":
    main()