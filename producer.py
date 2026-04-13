"""Replay preprocessed marathon events into Kafka in simulated real time.

Reads the time-sorted events CSV produced by preprocess.py and publishes each
event to a Kafka topic. Between events the script sleeps for an interval
proportional to the gap in elapsed race time, divided by the SPEEDUP factor
in config, so the stream behaves like a live race feed compressed (or
expanded) by a chosen factor.

Each message is keyed by race_id, which causes all events belonging to the
same race to land on the same partition and be delivered in the order they
were sent.
"""
import json
import time

import pandas as pd
from kafka import KafkaProducer

from config import KAFKA_BOOTSTRAP, TOPIC, RACE_START_EPOCH, SPEEDUP

EVENTS_PATH = "data/events.csv"

# How often to print a progress line, measured in events sent
PROGRESS_INTERVAL = 1000


def build_event(row, replay_timestamp: float) -> dict:
    """Convert one CSV row into the event dictionary published to Kafka."""
    elapsed = int(row["elapsed_seconds"])
    return {
        "race_id": row["race_id"],
        "runner_id": str(row["runner_id"]),
        "runner_name": row["runner_name"],
        "age": None if pd.isna(row["age"]) else int(row["age"]),
        "gender": row["gender"],
        "checkpoint_label": row["checkpoint_label"],
        "checkpoint_km": float(row["checkpoint_km"]),
        "elapsed_seconds": elapsed,
        "event_timestamp": RACE_START_EPOCH + elapsed,
        "replay_timestamp": replay_timestamp,
    }


def main():
    """Load the events file and stream every row into Kafka."""
    events = pd.read_csv(EVENTS_PATH)
    print(f"Loaded {len(events):,} events. Replaying at {SPEEDUP}x speed.")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        linger_ms=5,
    )

    previous_elapsed = 0
    sent = 0
    start_wall_time = time.time()

    for _, row in events.iterrows():
        elapsed = int(row["elapsed_seconds"])
        gap = max(0, elapsed - previous_elapsed)
        if gap:
            time.sleep(gap / SPEEDUP)
        previous_elapsed = elapsed

        event = build_event(row, replay_timestamp=time.time())
        producer.send(TOPIC, key=event["race_id"], value=event)
        sent += 1

        if sent % PROGRESS_INTERVAL == 0:
            rate = sent / (time.time() - start_wall_time)
            print(
                f"  sent {sent:,} events "
                f"({rate:.0f}/s, race_clock={previous_elapsed}s)"
            )

    producer.flush()
    producer.close()

    duration = time.time() - start_wall_time
    print(f"Done. Sent {sent:,} events in {duration:.1f}s.")


if __name__ == "__main__":
    main()