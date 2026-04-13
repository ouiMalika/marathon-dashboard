# Real-Time Marathon Analytics Pipeline

A streaming data pipeline that replays historical Boston Marathon checkpoint
data as a Kafka event stream, processes it with a stateful Python consumer,
and serves live race metrics through a Streamlit dashboard.

## Architecture

```
results.csv → preprocess → producer → Kafka → consumer → SQLite → Streamlit
                                    (3 partitions, keyed by race_id)
```

| Layer | Component | Role |
|---|---|---|
| Preparation | `preprocess.py` | Melts the wide results CSV into time-sorted events |
| Ingestion | `producer.py` | Publishes events to Kafka in simulated real time |
| Messaging | Kafka (Docker) | Partitioned, durable event log |
| Processing | `consumer.py` | Stateful aggregation written to SQLite |
| Storage | `marathon.db` | Raw event log and seven aggregate tables |
| Presentation | `dashboard.py` | Streamlit dashboard polling SQLite every two seconds |

## Live metrics

1. **Leaderboard per checkpoint** — running minimum elapsed time
2. **Checkpoint throughput** — runners crossing each checkpoint per five-minute window
3. **Rolling segment pace** — average pace across the most recent 50 runners per segment
4. **Completion counter** — running total of runners past each checkpoint
5. **Pace ratio classification** — finishers grouped by whether their second half was faster, equal to, or slower than their first half
6. **Cohort pace decay** — average pace per segment grouped by runner cohort (elite, competitive, recreational, back of pack)
7. **Data quality flags** — segments with paces outside the physically plausible range

## Stack

Python 3.11+ · Apache Kafka · kafka-python-ng · SQLite · pandas · Streamlit · Docker Compose

## Dataset

[Boston Marathon results](https://www.kaggle.com/datasets/rojour/boston-results) on Kaggle.
Around 237,000 checkpoint events per race year after the wide-to-long transformation.

## Setup

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
docker-compose up -d

# Place marathon_results_2015.csv at data/results.csv, then:
python preprocess.py
python create_topic.py
```

## Run

Either launch the full pipeline with one command:

```bash
python run_live.py
```

Or run the three components manually in separate terminals (venv activated in each):

```bash
python consumer.py          # terminal 1, start first
python producer.py          # terminal 2
streamlit run dashboard.py  # terminal 3, opens http://localhost:8501
```

Adjust `SPEEDUP` in `config.py` to control replay speed. `1` is real time
(roughly four hours), `60` compresses the race into about four minutes, and
`600` runs through the full event stream in about twenty-five seconds.

## Design notes

- **Partitioning by `race_id`** keeps every event for a single race on the same Kafka partition, which preserves the order in which checkpoints were crossed. With multiple races, Kafka's default murmur2 partitioner spreads them across partitions automatically and consumers in the same group can be added for parallelism.
- **SQLite** is used as the streaming sink to keep the project free of external dependencies. The schema and upsert syntax are standard SQL, so moving to Postgres would mostly be a connection string change.
- **Idempotency** is enforced through a uniqueness constraint on `(race_id, runner_id, checkpoint_label)` in the raw event log, and through upsert statements on every aggregate table.
- **In-memory consumer state** is sufficient at this scale. A production deployment would use a stream processing framework such as Kafka Streams or Apache Flink with checkpointed state stores for fault tolerance.

## Project layout

```
marathon-pipeline/
├── README.md
├── requirements.txt
├── docker-compose.yml
├── config.py              shared configuration constants
├── preprocess.py          CSV transformation
├── create_topic.py        Kafka topic provisioning
├── producer.py            event replay into Kafka
├── consumer.py            stateful aggregation into SQLite
├── dashboard.py           Streamlit live view
├── run_live.py            convenience launcher
└── data/
    └── results.csv        provided by the user, not committed
```
