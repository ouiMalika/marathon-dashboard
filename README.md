# Real-Time Marathon Analytics Pipeline

A streaming data pipeline that replays historical Boston Marathon checkpoint data
as a Kafka event stream, processes it with a stateful Python consumer, and serves
live race metrics through a Streamlit dashboard.

## Architecture

```
results.csv → preprocess → producer → Kafka → consumer → SQLite → Streamlit
                                    (3 partitions, keyed by race_id)
```

| Layer | Component | Role |
|---|---|---|
| Prep | `preprocess.py` | Melts wide CSV into time-sorted events |
| Ingestion | `producer.py` | Replays events into Kafka in simulated real time |
| Messaging | Kafka (Docker) | Partitioned, durable event log |
| Processing | `consumer.py` | Stateful aggregation → SQLite |
| Storage | `marathon.db` | Raw events + 4 aggregate tables (upsert) |
| Presentation | `dashboard.py` | Streamlit, polls every 2s |

## Live metrics

1. Leaderboard per checkpoint (running min)
2. Checkpoint throughput (5-min tumbling windows)
3. Rolling segment pace (last 50 runners per segment)
4. Completion counter per checkpoint

## Stack

Python 3.11+ · Apache Kafka · kafka-python-ng · SQLite · pandas · Streamlit · Docker Compose

## Dataset

[Boston Marathon results](https://www.kaggle.com/datasets/rojour/boston-results) on Kaggle.
~237k checkpoint events per race year after melting wide → long.

## Setup

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
docker-compose up -d

# Drop marathon_results_2015.csv into data/results.csv, then:
python preprocess.py
python create_topic.py
```

## Run

Three terminals (venv activated in each):

```bash
python consumer.py          # terminal 1 — start first
python producer.py          # terminal 2
streamlit run dashboard.py  # terminal 3 — opens localhost:8501
```

Adjust `SPEEDUP` in `config.py` to control replay speed (1 = real time, 600 = ~25s demo).

## Design notes

- **Partition by `race_id`** preserves checkpoint ordering for leaderboard correctness; runner-level keying would fragment ordering across partitions.
- **SQLite** keeps the project standalone; swapping to Postgres is a connection-string change.
- **Idempotency** via `UNIQUE(race_id, runner_id, checkpoint_km)` + `INSERT OR IGNORE` on raw events.
- **In-memory consumer state** is sufficient at this scale; production would use Kafka Streams or Flink with checkpointed state.
